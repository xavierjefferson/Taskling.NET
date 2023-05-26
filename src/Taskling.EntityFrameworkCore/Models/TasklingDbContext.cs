using Microsoft.EntityFrameworkCore;

namespace Taskling.EntityFrameworkCore.Models;

public partial class TasklingDbContext : DbContext
{
    public TasklingDbContext()
    {
    }

    public TasklingDbContext(DbContextOptions<TasklingDbContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Block> Blocks { get; set; }
    public virtual DbSet<BlockExecution> BlockExecutions { get; set; }
    public virtual DbSet<ForcedBlockQueue> ForcedBlockQueues { get; set; }
    public virtual DbSet<ListBlockItem> ListBlockItems { get; set; }
    public virtual DbSet<TaskDefinition> TaskDefinitions { get; set; }
    public virtual DbSet<TaskExecution> TaskExecutions { get; set; }
    public virtual DbSet<TaskExecutionEvent> TaskExecutionEvents { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!optionsBuilder.IsConfigured)
        {
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https: //go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see http://go.microsoft.com/fwlink/?LinkId=723263.
            optionsBuilder.UseSqlServer("Server=.;Database=tasklingdb;Trusted_Connection=True;");
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Block>(entity =>
        {
            entity.ToTable("Block");

            entity.HasIndex(e => e.TaskDefinitionId, "IX_Block_TaskDefinitionId");

            entity.Property(e => e.CreatedDate);

            entity.Property(e => e.FromDate);

            entity.Property(e => e.ToDate);

            entity.HasOne(d => d.TaskDefinition)
                .WithMany(p => p.Blocks)
                .HasForeignKey(d => d.TaskDefinitionId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK_Block_TaskDefinition");
        });

        modelBuilder.Entity<BlockExecution>(entity =>
        {
            entity.ToTable("BlockExecution");

            entity.HasIndex(e => e.BlockId, "IX_BlockExecution_BlockId");

            entity.HasIndex(e => e.TaskExecutionId, "IX_BlockExecution_TaskExecutionId");

            entity.Property(e => e.CompletedAt);

            entity.Property(e => e.CreatedAt);

            entity.Property(e => e.StartedAt);

            entity.HasOne(d => d.TaskExecution)
                .WithMany(p => p.BlockExecutions)
                .HasForeignKey(d => d.TaskExecutionId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK_BlockExecution_TaskExecution");

            entity.HasOne(d => d.Block)
                .WithMany(p => p.BlockExecutions)
                .HasForeignKey(d => d.BlockId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK_BlockExecution_Block");
        });

        modelBuilder.Entity<ForcedBlockQueue>(entity =>
        {
            entity.ToTable("ForcedBlockQueue");

            entity.Property(e => e.ForcedBy)
                .IsRequired()
                .HasMaxLength(50);

            entity.Property(e => e.ForcedDate);

            entity.Property(e => e.ProcessingStatus)
                .IsRequired().HasMaxLength(20);

            entity.HasOne(d => d.Block)
                .WithMany(p => p.ForcedBlockQueues)
                .HasForeignKey(d => d.BlockId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK_ForcedBlockQueue_Block");
        });

        modelBuilder.Entity<ListBlockItem>(entity =>
        {
            entity.ToTable("ListBlockItem");

            entity.Property(e => e.LastUpdated);

            entity.Property(e => e.Timestamp);

            entity.HasOne(d => d.Block)
                .WithMany(p => p.ListBlockItems)
                .HasForeignKey(d => d.BlockId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK_ListBlockItem_Block");
        });

        modelBuilder.Entity<TaskDefinition>(entity =>
        {
            entity.ToTable("TaskDefinition");

            entity.HasIndex(e => new { e.ApplicationName, e.TaskName }, "IX_TaskDefinition_Unique")
                .IsUnique();

            entity.Property(e => e.ApplicationName)
                .IsRequired()
                .HasMaxLength(200);

            entity.Property(e => e.ClientCsQueue);

            entity.Property(e => e.ExecutionTokens);

            entity.Property(e => e.LastCleaned);

            entity.Property(e => e.TaskName)
                .IsRequired()
                .HasMaxLength(200);

            entity.Property(e => e.UserCsQueue);
        });

        modelBuilder.Entity<TaskExecution>(entity =>
        {
            entity.ToTable("TaskExecution");

            entity.HasIndex(e => e.TaskDefinitionId, "IX_TaskExecution_TaskDefinitionId");

            entity.Property(e => e.CompletedAt);

            entity.Property(e => e.ExecutionHeader);

            entity.Property(e => e.LastKeepAlive);

            entity.Property(e => e.ReferenceValue).IsRequired();

            entity.Property(e => e.ServerName)
                .IsRequired()
                .HasMaxLength(200)
                ;

            entity.Property(e => e.StartedAt);

            entity.Property(e => e.TasklingVersion)
                .HasMaxLength(50)
                ;

            entity.HasOne(d => d.TaskDefinition)
                .WithMany(p => p.TaskExecutions)
                .HasForeignKey(d => d.TaskDefinitionId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK_TaskExecution_TaskDefinition");
        });

        modelBuilder.Entity<TaskExecutionEvent>(entity =>
        {
            entity.ToTable("TaskExecutionEvent");

            entity.Property(e => e.EventDateTime);

            entity.HasOne(d => d.TaskExecution)
                .WithMany(p => p.TaskExecutionEvents)
                .HasForeignKey(d => d.TaskExecutionId)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK_TaskExecutionEvent_TaskExecution");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}